package mediaworker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/media"
	"iot-platform/internal/mediaprotocol"
)

type repositoryStub struct {
	record        media.Record
	admitErr      error
	reserveStat   media.GrantStatus
	reservation   media.GrantReservation
	confirmOK     bool
	completeErr   error
	admitCalls    int
	completeCalls int
}

func (r *repositoryStub) Admit(context.Context, media.Admission) (media.Record, error) {
	r.admitCalls++
	if r.admitErr != nil {
		return media.Record{}, r.admitErr
	}
	return r.record, nil
}
func (r *repositoryStub) ReserveGrant(context.Context, media.GrantRequest) (media.GrantReservation, media.GrantStatus, error) {
	return r.reservation, r.reserveStat, nil
}
func (r *repositoryStub) ConfirmGrant(context.Context, media.GrantReservation, time.Time) (bool, error) {
	return r.confirmOK, nil
}
func (r *repositoryStub) Complete(context.Context, string, string) (media.Record, error) {
	r.completeCalls++
	return r.record, r.completeErr
}

type signerStub struct {
	path  string
	grant SignedGrant
	err   error
}

func (s *signerStub) SignUpload(_ context.Context, path string) (SignedGrant, error) {
	s.path = path
	return s.grant, s.err
}

type publisherStub struct {
	grants  []mediaprotocol.UploadGrant
	results []mediaprotocol.UploadResult
}

func (p *publisherStub) Publish(_ context.Context, m any) error {
	switch v := m.(type) {
	case mediaprotocol.UploadGrant:
		p.grants = append(p.grants, v)
	case mediaprotocol.UploadResult:
		p.results = append(p.results, v)
	}
	return nil
}

func TestRequestSignsOnlyCommittedPathAfterConfirm(t *testing.T) {
	expires := time.Now().Add(time.Minute).UTC().Truncate(time.Second)
	repo := &repositoryStub{
		record:      media.Record{MediaID: testMediaID, GatewayID: "gw1", UploadID: "u1", StoragePath: "committed.mp4", ValidationStatus: "pending"},
		reservation: media.GrantReservation{GatewayID: "gw1", UploadID: "u1", MediaID: testMediaID, GrantGeneration: 1, ReservedUntil: expires.Add(time.Minute)},
		reserveStat: media.GrantNeedUpload, confirmOK: true,
	}
	signer := &signerStub{grant: SignedGrant{URL: testSignedURL, ExpiresAt: expires}}
	pub := &publisherStub{}
	h := newTestHandler(repo, signer, pub)
	if err := h.Handle(context.Background(), delivery(`{"version":1,"upload_id":"u1","captured_at":"2026-11-20T10:00:00Z","content_type":"video/mp4","size_bytes":20,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)); err != nil {
		t.Fatalf("handle request: %v", err)
	}
	if signer.path != "committed.mp4" || len(pub.grants) != 1 || pub.grants[0].Status != mediaprotocol.GrantNeedUpload || pub.grants[0].UploadURL != testSignedURL {
		t.Fatalf("sign path=%q grants=%+v", signer.path, pub.grants)
	}
}

func TestRequestNoGrantOnDependencyOrRevocationAndDuplicateFence(t *testing.T) {
	tests := []struct {
		name string
		repo *repositoryStub
		want string
	}{
		{"dependency", &repositoryStub{admitErr: media.ErrDependency}, ""},
		{"revoked", &repositoryStub{admitErr: media.ErrForbidden}, ""},
		{"possible put", &repositoryStub{record: media.Record{MediaID: testMediaID}, reserveStat: media.GrantRetryAfterCompletion}, mediaprotocol.GrantRetryAfterCompletion},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			signer, pub := &signerStub{}, &publisherStub{}
			err := newTestHandler(tt.repo, signer, pub).Handle(context.Background(), delivery(requestPayload))
			if tt.name == "revoked" {
				if !errors.Is(err, media.ErrForbidden) {
					t.Fatalf("handle revoked err=%v, want ErrForbidden", err)
				}
			} else if err != nil {
				t.Fatalf("handle err=%v, want nil", err)
			}
			if len(pub.grants) != boolInt(tt.want != "") || (tt.want != "" && pub.grants[0].Status != tt.want) || signer.path != "" {
				t.Fatalf("grants=%+v path=%q", pub.grants, signer.path)
			}
			if tt.want == mediaprotocol.GrantRetryAfterCompletion && pub.grants[0].MediaID != testMediaID.String() {
				t.Fatalf("retry_after_completion media_id=%q, want %q", pub.grants[0].MediaID, testMediaID.String())
			}
		})
	}
}

func TestCompleteReplayPublishesDurableReady(t *testing.T) {
	repo := &repositoryStub{record: media.Record{MediaID: testMediaID, ValidationStatus: "ready"}}
	pub := &publisherStub{}
	if err := newTestHandler(repo, &signerStub{}, pub).Handle(context.Background(), completion("u1")); err != nil {
		t.Fatalf("handle completion: %v", err)
	}
	if len(pub.results) != 1 || pub.results[0].Status != mediaprotocol.ResultReady || pub.results[0].MediaID != testMediaID.String() {
		t.Fatalf("results=%+v", pub.results)
	}
}

func TestResultFromRecordReplaysMissingInstruction(t *testing.T) {
	rec := media.Record{MediaID: testMediaID, ValidationStatus: "pending", LastVerificationError: "OBJECT_MISSING"}
	result := resultFromRecord("gw1", "u1", rec)
	if result.Status != mediaprotocol.ResultPending || result.Code != "OBJECT_MISSING" || !result.Retryable || result.MediaID != testMediaID.String() {
		t.Fatalf("missing-object replay = %+v", result)
	}
}

func TestRejectedAndExpiredRequestReplayPublishesResult(t *testing.T) {
	for _, state := range []string{"rejected", "expired"} {
		t.Run(state, func(t *testing.T) {
			repo := &repositoryStub{record: media.Record{MediaID: testMediaID, ValidationStatus: state}}
			pub := &publisherStub{}
			if err := newTestHandler(repo, &signerStub{}, pub).Handle(context.Background(), delivery(requestPayload)); err != nil {
				t.Fatalf("handle replay: %v", err)
			}
			if len(pub.results) != 1 || len(pub.grants) != 0 {
				t.Fatalf("results=%+v grants=%+v", pub.results, pub.grants)
			}
		})
	}
}

func TestReadyRequestReplayAndFailedConfirmNeverPublishURL(t *testing.T) {
	readyRepo := &repositoryStub{record: media.Record{MediaID: testMediaID, ValidationStatus: "ready"}}
	readyPub := &publisherStub{}
	if err := newTestHandler(readyRepo, &signerStub{}, readyPub).Handle(context.Background(), delivery(requestPayload)); err != nil {
		t.Fatalf("handle ready replay: %v", err)
	}
	if len(readyPub.results) != 1 || readyPub.results[0].Status != mediaprotocol.ResultReady {
		t.Fatalf("ready replay results=%+v", readyPub.results)
	}

	expires := time.Now().Add(time.Minute)
	repo := &repositoryStub{
		record:      media.Record{MediaID: testMediaID, StoragePath: "committed.mp4", ValidationStatus: "pending"},
		reservation: media.GrantReservation{GatewayID: "gw1", UploadID: "u1", MediaID: testMediaID, GrantGeneration: 1, ReservedUntil: expires.Add(time.Minute)},
		reserveStat: media.GrantNeedUpload, confirmOK: false,
	}
	pub := &publisherStub{}
	if err := newTestHandler(repo, &signerStub{grant: SignedGrant{URL: testSignedURL, ExpiresAt: expires}}, pub).Handle(context.Background(), delivery(requestPayload)); err != nil {
		t.Fatalf("handle unconfirmed grant: %v", err)
	}
	if len(pub.grants) != 0 {
		t.Fatalf("published unconfirmed grant: %+v", pub.grants)
	}
}

func TestHandlerRejectsForeignTopicAndWorkerLifecycleIsBounded(t *testing.T) {
	pub := &publisherStub{}
	h := newTestHandler(&repositoryStub{}, &signerStub{}, pub)
	input := make(chan Delivery)
	if err := h.Start(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	h.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Handle(context.Background(), Delivery{Topic: "gateways/gw1/media/up/other", Payload: []byte(`{"version":1,"upload_id":"u1"}`)}); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("expected rejected topic, got %v", err)
	}
	if len(pub.results) != 0 {
		t.Fatal("unexpected publication")
	}
}

var testMediaID = uuid.MustParse("23456789-abcd-4abc-8def-0123456789ab")

func newTestHandler(repo Repository, signer Signer, pub Publisher) *Handler {
	return New(repo, signer, pub, Options{Workers: 1, SigningTimeout: time.Second, GrantTTL: time.Minute, InFlightGrace: time.Second})
}
func delivery(payload string) Delivery {
	return Delivery{Topic: mediaprotocol.UploadRequestTopic("gw1"), Payload: []byte(payload)}
}
func completion(id string) Delivery {
	return Delivery{Topic: mediaprotocol.UploadCompleteTopic("gw1"), Payload: []byte(`{"version":1,"upload_id":"` + id + `"}`)}
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

const requestPayload = `{"version":1,"upload_id":"u1","captured_at":"2026-11-20T10:00:00Z","content_type":"video/mp4","size_bytes":20,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`
const testSignedURL = "https://storage.example/storage/v1/object/upload/sign/media-video/123.mp4?token=t"
