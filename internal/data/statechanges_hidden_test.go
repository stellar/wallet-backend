package data

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func Test_hiddenTokensCondition(t *testing.T) {
	m := &StateChangeModel{}
	cond, args, argIndex := m.hiddenTokensCondition("token_id", "state_change_id", []interface{}{"a"}, 2)
	assert.Empty(t, cond)
	assert.Equal(t, []interface{}{"a"}, args)
	assert.Equal(t, 2, argIndex)

	m.HiddenTokenRanges = []HiddenTokenRange{{TokenID: []byte{1}, FromID: 10, ToID: 20}, {TokenID: []byte{2}, FromID: 30, ToID: 40}}
	cond, args, argIndex = m.hiddenTokensCondition("sc.token_id", "sc.state_change_id", []interface{}{"a"}, 2)
	assert.Equal(t, " AND (sc.token_id IS NULL OR (NOT (sc.token_id = $2 AND sc.state_change_id >= $3 AND sc.state_change_id < $4) AND NOT (sc.token_id = $5 AND sc.state_change_id >= $6 AND sc.state_change_id < $7)))", cond)
	assert.Equal(t, []interface{}{"a", []byte{1}, int64(10), int64(20), []byte{2}, int64(30), int64(40)}, args)
	assert.Equal(t, 8, argIndex)
}

// Every state_changes read skips a hidden token's rows inside the hidden
// range, keeps the same token's rows outside it, and keeps rows with no token.
func TestStateChangeModel_HiddenTokenRanges(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	// The API server's pool runs in pgx's Exec query mode; test with the same
	// parameter encoding.
	poolCfg := db.DefaultPoolConfig()
	poolCfg.QueryExecMode = pgx.QueryExecModeExec
	pool, err := db.OpenDBConnectionPool(ctx, dbt.DSN, poolCfg)
	require.NoError(t, err)
	defer pool.Close()

	const hidden = "CAS3FL6TLZKDGGSISDBWGGPXT3NRR4DYTZD7YOD3HMYO6LTJUVGRVEAM"
	const tracked = "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
	tokenIDOf := func(addr string) []byte {
		v, err := types.AddressBytea(addr).Value()
		require.NoError(t, err)
		return v.([]byte)
	}
	indexerRange := func(addr string) HiddenTokenRange {
		return HiddenTokenRange{TokenID: tokenIDOf(addr), FromID: types.StateChangeOrdinalBaseIndexer, ToID: types.StateChangeOrdinalBaseIndexer + types.StateChangeOrdinalNamespaceWidth}
	}
	sep41Range := func(addr string) HiddenTokenRange {
		return HiddenTokenRange{TokenID: tokenIDOf(addr), FromID: types.StateChangeOrdinalBaseSEP41, ToID: types.StateChangeOrdinalBaseSEP41 + types.StateChangeOrdinalNamespaceWidth}
	}

	now := time.Now()
	account := keypair.MustRandom().Address()
	// Operation ids carry their transaction's to_id in the high bits.
	const toID = int64(1 << 12)
	sep41Row := types.StateChangeOrdinalBaseSEP41 + 1
	_, err = pool.Exec(ctx, `
		INSERT INTO state_changes (to_id, operation_id, state_change_id, state_change_category, state_change_reason, ledger_created_at, ledger_number, account_id, token_id)
		VALUES
			($1, $6, 1, 'BALANCE', 'CREDIT', $2, 1, $3, $4),
			($1, $7, 2, 'BALANCE', 'CREDIT', $2, 1, $3, $5),
			($1, $8, 3, 'BALANCE', 'CREDIT', $2, 1, $3, NULL),
			($1, $9, $10, 'BALANCE', 'CREDIT', $2, 1, $3, $5)
	`, toID, now, types.AddressBytea(account), types.AddressBytea(hidden), types.AddressBytea(tracked), toID+1, toID+2, toID+3, toID+4, sep41Row)
	require.NoError(t, err)

	metricsDB := metrics.NewMetrics(prometheus.NewRegistry()).DB
	ids := func(scs []*types.StateChangeWithCursor) []int64 {
		out := make([]int64, 0, len(scs))
		for _, sc := range scs {
			out = append(out, sc.StateChange.StateChangeID)
		}
		return out
	}
	all := []int64{1, 2, 3, sep41Row}

	t.Run("nothing hidden returns every row", func(t *testing.T) {
		m := &StateChangeModel{DB: pool, Metrics: metricsDB}
		scs, err := m.BatchGetByAccountAddress(ctx, account, nil, nil, nil, nil, "", nil, nil, ASC, nil)
		require.NoError(t, err)
		assert.Equal(t, all, ids(scs))
	})

	// The hidden token's only rows are in the indexer range; the tracked token
	// has rows in both ranges and only its SEP-41 row is hidden.
	m := &StateChangeModel{DB: pool, Metrics: metricsDB, HiddenTokenRanges: []HiddenTokenRange{indexerRange(hidden), sep41Range(tracked)}}
	visible := []int64{2, 3}

	t.Run("by account", func(t *testing.T) {
		scs, err := m.BatchGetByAccountAddress(ctx, account, nil, nil, nil, nil, "", nil, nil, ASC, nil)
		require.NoError(t, err)
		assert.Equal(t, visible, ids(scs))
	})

	t.Run("by account, paginated", func(t *testing.T) {
		limit := int32(1)
		page1, err := m.BatchGetByAccountAddress(ctx, account, nil, nil, nil, nil, "", &limit, nil, ASC, nil)
		require.NoError(t, err)
		require.Equal(t, []int64{2}, ids(page1))

		last := page1[0].StateChange
		cursor := &types.StateChangeCursor{LedgerCreatedAt: last.LedgerCreatedAt, ToID: last.ToID, OperationID: last.OperationID, StateChangeID: last.StateChangeID}
		page2, err := m.BatchGetByAccountAddress(ctx, account, nil, nil, nil, nil, "", &limit, cursor, ASC, nil)
		require.NoError(t, err)
		assert.Equal(t, []int64{3}, ids(page2))
	})

	t.Run("by transaction", func(t *testing.T) {
		scs, err := m.BatchGetByToID(ctx, toID, now, "", nil, nil, ASC)
		require.NoError(t, err)
		assert.Equal(t, visible, ids(scs))
	})

	t.Run("by transactions", func(t *testing.T) {
		scs, err := m.BatchGetByToIDs(ctx, []int64{toID}, []time.Time{now}, "", nil, ASC)
		require.NoError(t, err)
		assert.Equal(t, visible, ids(scs))

		// The LIMIT placeholder comes after the hidden-token arguments.
		limit := int32(1)
		scs, err = m.BatchGetByToIDs(ctx, []int64{toID}, []time.Time{now}, "", &limit, ASC)
		require.NoError(t, err)
		assert.Equal(t, []int64{2}, ids(scs))
	})

	t.Run("by operation", func(t *testing.T) {
		hiddenRowOnly, err := m.BatchGetByOperationID(ctx, toID+1, now, "", nil, nil, ASC)
		require.NoError(t, err)
		assert.Empty(t, hiddenRowOnly)

		shown, err := m.BatchGetByOperationID(ctx, toID+2, now, "", nil, nil, ASC)
		require.NoError(t, err)
		assert.Equal(t, []int64{2}, ids(shown))
	})

	t.Run("by operations", func(t *testing.T) {
		ops := []int64{toID + 1, toID + 2, toID + 3, toID + 4}
		times := []time.Time{now, now, now, now}
		scs, err := m.BatchGetByOperationIDs(ctx, ops, times, "", nil, ASC)
		require.NoError(t, err)
		assert.Equal(t, visible, ids(scs))

		limit := int32(1)
		scs, err = m.BatchGetByOperationIDs(ctx, ops, times, "", &limit, ASC)
		require.NoError(t, err)
		assert.Equal(t, visible, ids(scs), "the limit applies per operation")
	})

	t.Run("account rows by transactions", func(t *testing.T) {
		scs, err := m.BatchGetAccountStateChangesByToIDs(ctx, account, []int64{toID}, []time.Time{now}, "")
		require.NoError(t, err)
		got := make([]int64, 0, len(scs))
		for _, sc := range scs {
			got = append(got, sc.StateChangeID)
		}
		assert.Equal(t, []int64{3, 2}, got)
	})
}
