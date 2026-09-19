package data

import (
	"bytes"
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestAccountModelBatchGetByToIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &AccountModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	address1 := keypair.MustRandom().Address()
	address2 := keypair.MustRandom().Address()
	toID1 := int64(1)
	toID2 := int64(2)

	// Insert test transactions first (hash is BYTEA, using valid 64-char hex strings)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	_, err = m.DB.Exec(ctx, "INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at) VALUES ($1, $2, 100, 'TransactionResultCodeTxSuccess', 1, NOW()), ($3, $4, 200, 'TransactionResultCodeTxSuccess', 2, NOW())", testHash1, toID1, testHash2, toID2)
	require.NoError(t, err)

	// Insert test transactions_accounts links
	_, err = m.DB.Exec(ctx, "INSERT INTO transactions_accounts (ledger_created_at, tx_to_id, account_id) VALUES (NOW(), $1, $2), (NOW(), $3, $4)",
		toID1, types.AddressBytea(address1), toID2, types.AddressBytea(address2))
	require.NoError(t, err)

	// Test BatchGetByToIDs function
	accounts, err := m.BatchGetByToIDs(ctx, []int64{toID1, toID2}, "")
	require.NoError(t, err)
	assert.Len(t, accounts, 2)

	// Verify accounts are returned with correct to_id
	addressSet := make(map[string]int64)
	for _, acc := range accounts {
		addressSet[string(acc.StellarAddress)] = acc.ToID
	}
	assert.Equal(t, toID1, addressSet[address1])
	assert.Equal(t, toID2, addressSet[address2])
}

func TestAccountModelBatchGetByOperationIDs(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	ctx := context.Background()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	m := &AccountModel{
		DB:      dbConnectionPool,
		Metrics: dbMetrics,
	}

	address1 := keypair.MustRandom().Address()
	address2 := keypair.MustRandom().Address()
	operationID1 := int64(123)
	operationID2 := int64(456)

	// Insert test transactions first (hash is BYTEA, using valid 64-char hex strings)
	testHash1 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000001")
	testHash2 := types.HashBytea("0000000000000000000000000000000000000000000000000000000000000002")
	_, err = m.DB.Exec(ctx, "INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at) VALUES ($1, 4096, 100, 'TransactionResultCodeTxSuccess', 1, NOW()), ($2, 8192, 200, 'TransactionResultCodeTxSuccess', 2, NOW())", testHash1, testHash2)
	require.NoError(t, err)

	// Insert test operations (IDs don't need to be in TOID range here since we're just testing operations_accounts links)
	xdr1 := types.XDRBytea([]byte("xdr1"))
	xdr2 := types.XDRBytea([]byte("xdr2"))
	_, err = m.DB.Exec(ctx, "INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at) VALUES ($1, 'PAYMENT', $3, 'op_success', true, 1, NOW()), ($2, 'PAYMENT', $4, 'op_success', true, 2, NOW())", operationID1, operationID2, xdr1, xdr2)
	require.NoError(t, err)

	// Insert test operations_accounts links (account_id is BYTEA)
	_, err = m.DB.Exec(ctx, "INSERT INTO operations_accounts (ledger_created_at, operation_id, account_id) VALUES (NOW(), $1, $2), (NOW(), $3, $4)",
		operationID1, types.AddressBytea(address1), operationID2, types.AddressBytea(address2))
	require.NoError(t, err)

	// Test BatchGetByOperationID function
	accounts, err := m.BatchGetByOperationIDs(ctx, []int64{operationID1, operationID2}, "")
	require.NoError(t, err)
	assert.Len(t, accounts, 2)

	// Verify accounts are returned with correct operation_id
	addressSet := make(map[string]int64)
	for _, acc := range accounts {
		addressSet[string(acc.StellarAddress)] = acc.OperationID
	}
	assert.Equal(t, operationID1, addressSet[address1])
	assert.Equal(t, operationID2, addressSet[address2])
}

func TestBuildAccountLinkCopyRows(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	later := now.Add(time.Minute)
	addresses := []string{keypair.MustRandom().Address(), keypair.MustRandom().Address()}
	slices.SortFunc(addresses, func(a, b string) int {
		return bytes.Compare(mustAddressCopyBytes(t, a), mustAddressCopyBytes(t, b))
	})
	lowAddr, highAddr := addresses[0], addresses[1]

	txs := []*types.Transaction{
		{ToID: 4096, LedgerCreatedAt: now},
		{ToID: 8192, LedgerCreatedAt: later},
	}
	idAndCreatedAt := func(tx *types.Transaction) (int64, time.Time) { return tx.ToID, tx.LedgerCreatedAt }
	addressesByID := map[int64]map[string]struct{}{
		4096: {lowAddr: {}, highAddr: {}},
		8192: {highAddr: {}},
	}

	rows, err := BuildAccountLinkCopyRows(nil, txs, idAndCreatedAt, addressesByID, make(types.AddressByteaMemo))
	require.NoError(t, err)

	// Rows come out in (account_id, id) order — the link table's primary-key order —
	// regardless of the randomized map iteration that produced them.
	want := [][]any{
		{pgtype.Timestamptz{Time: now, Valid: true}, pgtype.Int8{Int64: 4096, Valid: true}, mustAddressCopyBytes(t, lowAddr)},
		{pgtype.Timestamptz{Time: now, Valid: true}, pgtype.Int8{Int64: 4096, Valid: true}, mustAddressCopyBytes(t, highAddr)},
		{pgtype.Timestamptz{Time: later, Valid: true}, pgtype.Int8{Int64: 8192, Valid: true}, mustAddressCopyBytes(t, highAddr)},
	}
	require.Len(t, rows, len(want))
	for i := range want {
		require.Len(t, rows[i], len(accountLinkCopyColumns("tx_to_id")), "row width must match the COPY column list")
		for j := range want[i] {
			assert.Equal(t, want[i][j], rows[i][j], "row %d column %s", i, accountLinkCopyColumns("tx_to_id")[j])
		}
	}
}

func TestBuildAccountLinkCopyRows_MissingParentRow(t *testing.T) {
	txs := []*types.Transaction{{ToID: 4096, LedgerCreatedAt: time.Now()}}
	idAndCreatedAt := func(tx *types.Transaction) (int64, time.Time) { return tx.ToID, tx.LedgerCreatedAt }
	addressesByID := map[int64]map[string]struct{}{
		8192: {keypair.MustRandom().Address(): {}},
	}

	_, err := BuildAccountLinkCopyRows(nil, txs, idAndCreatedAt, addressesByID, make(types.AddressByteaMemo))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no row supplies ledger_created_at for 8192")
}

func TestBuildAccountLinkCopyRows_EmptyInput(t *testing.T) {
	idAndCreatedAt := func(tx *types.Transaction) (int64, time.Time) { return tx.ToID, tx.LedgerCreatedAt }
	rows, err := BuildAccountLinkCopyRows(nil, []*types.Transaction{}, idAndCreatedAt, map[int64]map[string]struct{}{}, make(types.AddressByteaMemo))
	require.NoError(t, err)
	assert.Empty(t, rows)
}
