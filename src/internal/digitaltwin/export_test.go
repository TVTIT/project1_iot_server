package digitaltwin

import (
	"context"

	"github.com/google/uuid"
)

// ReadTemporalHistoryInternalForTest exposes the private readTemporalHistoryInternal execution seam
// to package digitaltwin_test for native integration tests exercising typed resolution policies.
func (r *PostgresTwinReader) ReadTemporalHistoryInternalForTest(
	ctx context.Context,
	actor uuid.UUID,
	entity ResolvedEntity,
	propertyName string,
	q TwinHistoryQuery,
) (*TwinHistoryResult, error) {
	return r.readTemporalHistoryInternal(ctx, actor, entity, propertyName, q)
}
