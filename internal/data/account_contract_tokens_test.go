package data

import (
	"context"
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestAccountContractTokensModel_GetSEP41ByAccountPaginated(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	sep41Addr1 := "CSEP41A1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	sep41Addr2 := "CSEP41A2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	sep41Addr3 := "CSEP41A3AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	sacAddr := "CSACAAAAAAQ2JQQF3NCRU7GYMDJNZ2NMQN6IGN4FCT5DWPODMPVEXSND"

	sep41ID1 := DeterministicContractID(sep41Addr1)
	sep41ID2 := DeterministicContractID(sep41Addr2)
	sep41ID3 := DeterministicContractID(sep41Addr3)
	sacID := DeterministicContractID(sacAddr)

	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO contract_tokens (id, contract_id, type, name, symbol, decimals) VALUES
		($1, $2, 'SEP41', 'Token One', 'ONE', 7),
		($3, $4, 'SEP41', 'Token Two', 'TWO', 7),
		($5, $6, 'SEP41', 'Token Three', 'THREE', 7),
		($7, $8, 'SAC', 'SAC Token', 'SAC', 7)
	`, sep41ID1, sep41Addr1, sep41ID2, sep41Addr2, sep41ID3, sep41Addr3, sacID, sacAddr)
	require.NoError(t, err)

	_, err = dbConnectionPool.Exec(ctx, `
		INSERT INTO account_contract_tokens (account_address, contract_id) VALUES
		('GACCOUNT1', $1),
		('GACCOUNT1', $2),
		('GACCOUNT1', $3),
		('GACCOUNT1', $4)
	`, sep41ID1, sep41ID2, sep41ID3, sacID)
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB
	m := &AccountContractTokensModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	expectedOrder := []string{sep41ID1.String(), sep41ID2.String(), sep41ID3.String()}
	slices.Sort(expectedOrder)

	limit := int32(2)
	page, err := m.GetSEP41ByAccountPaginated(ctx, "GACCOUNT1", &limit, nil, ASC)
	require.NoError(t, err)
	require.Len(t, page, 2)
	require.Equal(t, expectedOrder[0], page[0].ID.String())
	require.Equal(t, expectedOrder[1], page[1].ID.String())

	cursor := page[1].ID
	nextPage, err := m.GetSEP41ByAccountPaginated(ctx, "GACCOUNT1", &limit, &cursor, ASC)
	require.NoError(t, err)
	require.Len(t, nextPage, 1)
	require.Equal(t, expectedOrder[2], nextPage[0].ID.String())

	descPage, err := m.GetSEP41ByAccountPaginated(ctx, "GACCOUNT1", &limit, nil, DESC)
	require.NoError(t, err)
	require.Len(t, descPage, 2)
	require.Equal(t, expectedOrder[2], descPage[0].ID.String())
	require.Equal(t, expectedOrder[1], descPage[1].ID.String())
}
