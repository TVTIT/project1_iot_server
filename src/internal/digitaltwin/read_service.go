package digitaltwin

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// readService coordinates Digital Twin read operations by delegating bounds validation,
// actor verification, and single-budget context scoping to ReadPreparation, followed by
// data reading from TwinReader.
type readService struct {
	preparation *ReadPreparation
	reader      TwinReader
}

var _ ReadService = (*readService)(nil)

// NewReadService creates a new ReadService implementation.
// It verifies that both preparation and reader dependencies are non-nil (rejecting typed nils).
func NewReadService(preparation *ReadPreparation, reader TwinReader) (*readService, error) {
	if isNilDependency(preparation) {
		return nil, fmt.Errorf("preparation is required")
	}
	if isNilDependency(reader) {
		return nil, fmt.Errorf("reader is required")
	}
	return &readService{
		preparation: preparation,
		reader:      reader,
	}, nil
}

// ListTwins executes membership-scoped entity listing within a unified deadline budget.
// If preparation returns an error (such as validation failure, nil actor, or context deadline expiry),
// no partial result is exposed.
func (s *readService) ListTwins(ctx context.Context, actor uuid.UUID, q TwinListQuery) (*TwinListResult, error) {
	if isNilDependency(s) || isNilDependency(s.preparation) || isNilDependency(s.reader) {
		return nil, ErrDependencyUnavailable
	}

	var result *TwinListResult
	err := s.preparation.PrepareList(ctx, actor, q, func(scopedCtx context.Context, validatedQ TwinListQuery) error {
		res, err := s.reader.ListEntities(scopedCtx, actor, validatedQ)
		if err != nil {
			return err
		}
		if res == nil {
			return fmt.Errorf("%w: reader returned nil result without error", ErrUnexpected)
		}
		result = res
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetTwinDetail executes entity resolution, membership recheck, and graph detail reading within a unified deadline budget.
// If preparation or reader fails, no partial result is exposed.
func (s *readService) GetTwinDetail(ctx context.Context, actor uuid.UUID, q TwinDetailQuery) (*TwinDetailResult, error) {
	if isNilDependency(s) || isNilDependency(s.preparation) || isNilDependency(s.reader) {
		return nil, ErrDependencyUnavailable
	}

	var result *TwinDetailResult
	err := s.preparation.PrepareDetail(ctx, actor, q, func(scopedCtx context.Context, entity ResolvedEntity, validatedQ TwinDetailQuery) error {
		res, err := s.reader.GetDetail(scopedCtx, actor, entity, validatedQ)
		if err != nil {
			return err
		}
		if res == nil {
			return fmt.Errorf("%w: reader returned nil result without error", ErrUnexpected)
		}
		result = res
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetTwinState executes entity resolution, membership recheck, and current reported/desired state reading within a unified deadline budget.
// If preparation or reader fails, no partial result is exposed.
func (s *readService) GetTwinState(ctx context.Context, actor uuid.UUID, q TwinStateQuery) (*TwinStateResult, error) {
	if isNilDependency(s) || isNilDependency(s.preparation) || isNilDependency(s.reader) {
		return nil, ErrDependencyUnavailable
	}

	var result *TwinStateResult
	err := s.preparation.PrepareState(ctx, actor, q, func(scopedCtx context.Context, entity ResolvedEntity, _ TwinStateQuery) error {
		res, err := s.reader.GetState(scopedCtx, actor, entity)
		if err != nil {
			return err
		}
		if res == nil {
			return fmt.Errorf("%w: reader returned nil result without error", ErrUnexpected)
		}
		result = res
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetTwinHistory returns ErrUnexpected in Task 4.3 as temporal history delegation is implemented in Task 4.4.
func (s *readService) GetTwinHistory(_ context.Context, _ uuid.UUID, _ TwinHistoryQuery) (*TwinHistoryResult, error) {
	return nil, ErrUnexpected
}
