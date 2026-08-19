package services

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
)

// stubRepairer is a configurable ProtocolCurrentStateRepair for engine tests.
// Behavior is injected per test via function fields; call counts are recorded
// under a mutex because the engine repairs units concurrently.
type stubRepairer struct {
	mu         sync.Mutex
	pages      [][]RepairUnit
	fetchTruth func(unit RepairUnit, attempt int) (Truth, uint32, error)
	apply      func(unit RepairUnit, attempt int) (RepairOutcome, error)
	fetchCalls map[RepairUnit]int
	applyCalls map[RepairUnit]int
}

func newStubRepairer(pages [][]RepairUnit) *stubRepairer {
	return &stubRepairer{
		pages:      pages,
		fetchCalls: map[RepairUnit]int{},
		applyCalls: map[RepairUnit]int{},
	}
}

func (r *stubRepairer) ListUnits(_ context.Context, _ RepairScope, cursor string, _ int) ([]RepairUnit, string, error) {
	page := 0
	if cursor != "" {
		if _, err := fmt.Sscanf(cursor, "%d", &page); err != nil {
			return nil, "", err
		}
	}
	if page >= len(r.pages) {
		return nil, "", nil
	}
	next := ""
	if page+1 < len(r.pages) {
		next = fmt.Sprintf("%d", page+1)
	}
	return r.pages[page], next, nil
}

func (r *stubRepairer) FetchTruth(_ context.Context, unit RepairUnit) (Truth, uint32, error) {
	r.mu.Lock()
	r.fetchCalls[unit]++
	attempt := r.fetchCalls[unit]
	r.mu.Unlock()
	return r.fetchTruth(unit, attempt)
}

func (r *stubRepairer) Apply(_ context.Context, _ pgx.Tx, unit RepairUnit, _ Truth, _ uint32) (RepairOutcome, error) {
	r.mu.Lock()
	r.applyCalls[unit]++
	attempt := r.applyCalls[unit]
	r.mu.Unlock()
	return r.apply(unit, attempt)
}

func newRepairFixture(t *testing.T, repairer ProtocolCurrentStateRepair, protocol data.Protocols) (context.Context, *protocolCurrentStateRepairService) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	dbPool, _ := setupTestDB(t)

	protocolsModel := data.NewProtocolsModelMock(t)
	if protocol.ID != "" {
		protocolsModel.On("GetByIDs", ctx, []string{protocol.ID}).Return([]data.Protocols{protocol}, nil)
	}

	svc := NewProtocolCurrentStateRepairService(dbPool, protocolsModel, map[string]ProtocolCurrentStateRepair{"testproto": repairer}, 4)
	return ctx, svc
}

func TestProtocolCurrentStateRepair_Run(t *testing.T) {
	readyProtocol := data.Protocols{ID: "testproto", ClassificationStatus: data.StatusSuccess, CurrentStateMigrationStatus: data.StatusSuccess}

	t.Run("errors on unregistered protocol", func(t *testing.T) {
		ctx, svc := newRepairFixture(t, newStubRepairer(nil), data.Protocols{})
		err := svc.Run(ctx, "unknown", RepairScope{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no current-state repairer registered")
	})

	t.Run("refuses while current-state migration is in progress", func(t *testing.T) {
		repairer := newStubRepairer([][]RepairUnit{{"u1"}})
		ctx, svc := newRepairFixture(t, repairer, data.Protocols{
			ID: "testproto", ClassificationStatus: data.StatusSuccess, CurrentStateMigrationStatus: data.StatusInProgress,
		})
		err := svc.Run(ctx, "testproto", RepairScope{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "migration is in progress")
		assert.Empty(t, repairer.fetchCalls)
	})

	t.Run("repairs all units across pages", func(t *testing.T) {
		repairer := newStubRepairer([][]RepairUnit{{"u1", "u2"}, {"u3"}})
		repairer.fetchTruth = func(RepairUnit, int) (Truth, uint32, error) { return "100", 50, nil }
		repairer.apply = func(RepairUnit, int) (RepairOutcome, error) { return RepairApplied, nil }

		ctx, svc := newRepairFixture(t, repairer, readyProtocol)

		require.NoError(t, svc.Run(ctx, "testproto", RepairScope{}))
		assert.Len(t, repairer.applyCalls, 3)
	})

	t.Run("an unchanged unit verifies in a single attempt", func(t *testing.T) {
		repairer := newStubRepairer([][]RepairUnit{{"insync"}})
		repairer.fetchTruth = func(RepairUnit, int) (Truth, uint32, error) { return "100", 50, nil }
		repairer.apply = func(RepairUnit, int) (RepairOutcome, error) { return RepairUnchanged, nil }

		ctx, svc := newRepairFixture(t, repairer, readyProtocol)
		require.NoError(t, svc.Run(ctx, "testproto", RepairScope{}))
		assert.Equal(t, 1, repairer.applyCalls["insync"], "verified-in-sync must not retry")
		assert.Equal(t, 1, repairer.fetchCalls["insync"])
	})

	t.Run("retries with fresh truth when apply loses the race", func(t *testing.T) {
		repairer := newStubRepairer([][]RepairUnit{{"u1"}})
		repairer.fetchTruth = func(_ RepairUnit, attempt int) (Truth, uint32, error) { return "100", uint32(50 + attempt), nil }
		repairer.apply = func(_ RepairUnit, attempt int) (RepairOutcome, error) {
			if attempt >= 3 {
				return RepairApplied, nil
			}
			return RepairStale, nil
		}

		ctx, svc := newRepairFixture(t, repairer, readyProtocol)
		require.NoError(t, svc.Run(ctx, "testproto", RepairScope{}))
		assert.Equal(t, 3, repairer.fetchCalls["u1"], "each retry must refetch truth")
		assert.Equal(t, 3, repairer.applyCalls["u1"])
	})

	t.Run("gives up on a unit that keeps losing and continues the run", func(t *testing.T) {
		repairer := newStubRepairer([][]RepairUnit{{"hot", "ok"}})
		repairer.fetchTruth = func(RepairUnit, int) (Truth, uint32, error) { return "100", 50, nil }
		repairer.apply = func(unit RepairUnit, _ int) (RepairOutcome, error) {
			if unit == RepairUnit("ok") {
				return RepairApplied, nil
			}
			return RepairStale, nil
		}

		ctx, svc := newRepairFixture(t, repairer, readyProtocol)
		require.NoError(t, svc.Run(ctx, "testproto", RepairScope{}))
		assert.Equal(t, maxRepairAttempts, repairer.applyCalls["hot"])
		assert.Equal(t, 1, repairer.applyCalls["ok"])
	})

	t.Run("skips units whose truth cannot be fetched", func(t *testing.T) {
		repairer := newStubRepairer([][]RepairUnit{{"archived", "ok"}})
		repairer.fetchTruth = func(unit RepairUnit, _ int) (Truth, uint32, error) {
			if unit == RepairUnit("archived") {
				return nil, 0, errors.New("simulation failed: contract archived")
			}
			return "100", 50, nil
		}
		repairer.apply = func(RepairUnit, int) (RepairOutcome, error) { return RepairApplied, nil }

		ctx, svc := newRepairFixture(t, repairer, readyProtocol)
		require.NoError(t, svc.Run(ctx, "testproto", RepairScope{}))
		assert.Zero(t, repairer.applyCalls["archived"])
		assert.Equal(t, 1, repairer.applyCalls["ok"])
	})

	t.Run("aborts on a hard apply error", func(t *testing.T) {
		repairer := newStubRepairer([][]RepairUnit{{"u1"}})
		repairer.fetchTruth = func(RepairUnit, int) (Truth, uint32, error) { return "100", 50, nil }
		repairer.apply = func(RepairUnit, int) (RepairOutcome, error) { return 0, errors.New("db exploded") }

		ctx, svc := newRepairFixture(t, repairer, readyProtocol)
		err := svc.Run(ctx, "testproto", RepairScope{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "db exploded")
	})
}
