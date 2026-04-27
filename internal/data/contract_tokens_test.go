package data

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// Compile-time check that pgx.Tx is still used in other tests
var _ pgx.Tx = (pgx.Tx)(nil)

func TestContractModel_GetExisting(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM contract_tokens`)
		require.NoError(t, err)
	}

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	t.Run("returns nil for empty input", func(t *testing.T) {
		m := &ContractModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			ids, txErr := m.GetExisting(ctx, dbTx, []string{})
			require.NoError(t, txErr)
			require.Nil(t, ids)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("returns empty slice when no matches", func(t *testing.T) {
		cleanUpDB()

		m := &ContractModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			ids, txErr := m.GetExisting(ctx, dbTx, []string{"nonexistent1", "nonexistent2"})
			require.NoError(t, txErr)
			require.Empty(t, ids)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("returns partial matches", func(t *testing.T) {
		cleanUpDB()

		m := &ContractModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		name := "Test"
		symbol := "TST"
		contracts := []*Contract{
			{ID: DeterministicContractID("contract1"), ContractID: "contract1", Type: "sac", Name: &name, Symbol: &symbol, Decimals: 7},
			{ID: DeterministicContractID("contract2"), ContractID: "contract2", Type: "sep41", Name: &name, Symbol: &symbol, Decimals: 18},
		}

		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			return nil
		})
		require.NoError(t, err)

		err = db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			ids, txErr := m.GetExisting(ctx, dbTx, []string{"contract1", "contract3", "contract4"})
			require.NoError(t, txErr)
			require.Len(t, ids, 1)
			require.Contains(t, ids, "contract1")
			return nil
		})
		require.NoError(t, err)

		cleanUpDB()
	})

	t.Run("returns all matches when all exist", func(t *testing.T) {
		cleanUpDB()

		m := &ContractModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		name := "Test"
		symbol := "TST"
		contracts := []*Contract{
			{ID: DeterministicContractID("contract1"), ContractID: "contract1", Type: "sac", Name: &name, Symbol: &symbol, Decimals: 7},
			{ID: DeterministicContractID("contract2"), ContractID: "contract2", Type: "sep41", Name: &name, Symbol: &symbol, Decimals: 18},
			{ID: DeterministicContractID("contract3"), ContractID: "contract3", Type: "sac", Name: &name, Symbol: &symbol, Decimals: 6},
		}

		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			return nil
		})
		require.NoError(t, err)

		err = db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			ids, txErr := m.GetExisting(ctx, dbTx, []string{"contract1", "contract2"})
			require.NoError(t, txErr)
			require.Len(t, ids, 2)
			require.ElementsMatch(t, []string{"contract1", "contract2"}, ids)
			return nil
		})
		require.NoError(t, err)

		cleanUpDB()
	})
}

func TestContractModel_BatchInsert(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM contract_tokens`)
		require.NoError(t, err)
	}

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	t.Run("returns success for empty contracts slice", func(t *testing.T) {
		m := &ContractModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			txErr := m.BatchInsert(ctx, dbTx, []*Contract{})
			require.NoError(t, txErr)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("returns success for multiple new contracts", func(t *testing.T) {
		cleanUpDB()

		m := &ContractModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		code1 := "TEST1"
		issuer1 := "GTEST123"
		name1 := "Test Contract 1"
		symbol1 := "TST1"
		code2 := "TEST2"
		issuer2 := "GTEST456"
		name2 := "Test Contract 2"
		symbol2 := "TST2"
		name3 := "Test Contract 3"
		symbol3 := "TST3"
		contracts := []*Contract{
			{
				ID:         DeterministicContractID("contract1"),
				ContractID: "contract1",
				Type:       "sac",
				Code:       &code1,
				Issuer:     &issuer1,
				Name:       &name1,
				Symbol:     &symbol1,
				Decimals:   7,
			},
			{
				ID:         DeterministicContractID("contract2"),
				ContractID: "contract2",
				Type:       "sep41",
				Code:       &code2,
				Issuer:     &issuer2,
				Name:       &name2,
				Symbol:     &symbol2,
				Decimals:   18,
			},
			{
				ID:         DeterministicContractID("contract3"),
				ContractID: "contract3",
				Type:       "unknown",
				Name:       &name3,
				Symbol:     &symbol3,
				Decimals:   6,
			},
		}

		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			return nil
		})
		require.NoError(t, err)

		// Verify contracts were inserted using direct SQL
		contract1, err := db.QueryOne[Contract](ctx, dbConnectionPool, `SELECT * FROM contract_tokens WHERE contract_id = $1`, "contract1")
		require.NoError(t, err)
		require.Equal(t, "sac", contract1.Type)
		require.Equal(t, "TEST1", *contract1.Code)
		require.Equal(t, "GTEST123", *contract1.Issuer)
		require.Equal(t, "Test Contract 1", *contract1.Name)
		require.Equal(t, "TST1", *contract1.Symbol)
		require.Equal(t, uint32(7), contract1.Decimals)

		contract2, err := db.QueryOne[Contract](ctx, dbConnectionPool, `SELECT * FROM contract_tokens WHERE contract_id = $1`, "contract2")
		require.NoError(t, err)
		require.Equal(t, "sep41", contract2.Type)

		contract3, err := db.QueryOne[Contract](ctx, dbConnectionPool, `SELECT * FROM contract_tokens WHERE contract_id = $1`, "contract3")
		require.NoError(t, err)
		require.Nil(t, contract3.Code)
		require.Nil(t, contract3.Issuer)

		cleanUpDB()
	})

	t.Run("accepts SEP-41 decimals values above SMALLINT range", func(t *testing.T) {
		cleanUpDB()

		m := &ContractModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		name := "Wide Decimals"
		symbol := "WIDE"
		// 40000 exceeds PostgreSQL SMALLINT max (32767) but is a valid SEP-41 u32.
		// Before widening contract_tokens.decimals to INTEGER, this insert would
		// fail with "40000 is greater than maximum value for int2" and roll back
		// the whole ledger-persistence transaction.
		const wideDecimals uint32 = 40000
		contracts := []*Contract{
			{
				ID:         DeterministicContractID("wide_contract"),
				ContractID: "wide_contract",
				Type:       "sep41",
				Name:       &name,
				Symbol:     &symbol,
				Decimals:   wideDecimals,
			},
		}

		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchInsert(ctx, dbTx, contracts)
		})
		require.NoError(t, err)

		stored, err := db.QueryOne[Contract](ctx, dbConnectionPool, `SELECT * FROM contract_tokens WHERE contract_id = $1`, "wide_contract")
		require.NoError(t, err)
		require.Equal(t, wideDecimals, stored.Decimals)

		cleanUpDB()
	})

	t.Run("skips duplicate contracts with ON CONFLICT DO NOTHING", func(t *testing.T) {
		cleanUpDB()

		m := &ContractModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		// First insert
		origName := "Original Name"
		origSymbol := "ORIG"
		contracts := []*Contract{
			{
				ID:         DeterministicContractID("contract1"),
				ContractID: "contract1",
				Type:       "sac",
				Name:       &origName,
				Symbol:     &origSymbol,
				Decimals:   7,
			},
		}

		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			return nil
		})
		require.NoError(t, err)

		// Second insert with same ContractID and different data - should be skipped (ON CONFLICT DO NOTHING)
		newName := "New Name"
		newSymbol := "NEW"
		contract2Name := "Contract 2"
		contract2Symbol := "C2"
		contracts = []*Contract{
			{
				ID:         DeterministicContractID("contract1"),
				ContractID: "contract1",
				Type:       "sep41",
				Name:       &newName,
				Symbol:     &newSymbol,
				Decimals:   18,
			},
			{
				ID:         DeterministicContractID("contract2"),
				ContractID: "contract2",
				Type:       "unknown",
				Name:       &contract2Name,
				Symbol:     &contract2Symbol,
				Decimals:   6,
			},
		}

		err = db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			txErr := m.BatchInsert(ctx, dbTx, contracts)
			require.NoError(t, txErr)
			return nil
		})
		require.NoError(t, err)

		// Verify original contract was not updated using direct SQL
		contract1, err := db.QueryOne[Contract](ctx, dbConnectionPool, `SELECT * FROM contract_tokens WHERE contract_id = $1`, "contract1")
		require.NoError(t, err)
		require.Equal(t, "sac", contract1.Type)
		require.Equal(t, "Original Name", *contract1.Name)
		require.Equal(t, "ORIG", *contract1.Symbol)
		require.Equal(t, uint32(7), contract1.Decimals)

		cleanUpDB()
	})
}

func TestContractModel_BatchUpdateMetadata(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM contract_tokens`)
		require.NoError(t, err)
	}

	reg := prometheus.NewRegistry()
	dbMetrics := metrics.NewMetrics(reg).DB

	t.Run("returns success for empty contracts slice", func(t *testing.T) {
		m := &ContractModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchUpdateMetadata(ctx, dbTx, []*Contract{})
		})
		require.NoError(t, err)
	})

	t.Run("updates type/name/symbol/decimals on existing rows", func(t *testing.T) {
		cleanUpDB()

		m := &ContractModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		origName1 := "Pre-classification 1"
		origSymbol1 := "PRE1"
		origName2 := "Pre-classification 2"
		origSymbol2 := "PRE2"
		seeded := []*Contract{
			{
				ID:         DeterministicContractID("contract1"),
				ContractID: "contract1",
				Type:       "unknown",
				Name:       &origName1,
				Symbol:     &origSymbol1,
				Decimals:   0,
			},
			{
				ID:         DeterministicContractID("contract2"),
				ContractID: "contract2",
				Type:       "unknown",
				Name:       &origName2,
				Symbol:     &origSymbol2,
				Decimals:   0,
			},
		}
		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchInsert(ctx, dbTx, seeded)
		})
		require.NoError(t, err)

		newName1 := "USD Coin"
		newSymbol1 := "USDC"
		newName2 := "Wrapped Ether"
		newSymbol2 := "WETH"
		updates := []*Contract{
			{
				ID:       DeterministicContractID("contract1"),
				Type:     "sep41",
				Name:     &newName1,
				Symbol:   &newSymbol1,
				Decimals: 7,
			},
			{
				ID:       DeterministicContractID("contract2"),
				Type:     "sep41",
				Name:     &newName2,
				Symbol:   &newSymbol2,
				Decimals: 18,
			},
		}
		err = db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchUpdateMetadata(ctx, dbTx, updates)
		})
		require.NoError(t, err)

		stored1, err := db.QueryOne[Contract](ctx, dbConnectionPool, `SELECT * FROM contract_tokens WHERE contract_id = $1`, "contract1")
		require.NoError(t, err)
		require.Equal(t, "sep41", stored1.Type)
		require.Equal(t, "USD Coin", *stored1.Name)
		require.Equal(t, "USDC", *stored1.Symbol)
		require.Equal(t, uint32(7), stored1.Decimals)

		stored2, err := db.QueryOne[Contract](ctx, dbConnectionPool, `SELECT * FROM contract_tokens WHERE contract_id = $1`, "contract2")
		require.NoError(t, err)
		require.Equal(t, "sep41", stored2.Type)
		require.Equal(t, "Wrapped Ether", *stored2.Name)
		require.Equal(t, "WETH", *stored2.Symbol)
		require.Equal(t, uint32(18), stored2.Decimals)

		cleanUpDB()
	})

	t.Run("writes NULL when name/symbol pointers are nil", func(t *testing.T) {
		cleanUpDB()

		m := &ContractModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		seedName := "Has Metadata"
		seedSymbol := "HAS"
		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchInsert(ctx, dbTx, []*Contract{
				{
					ID:         DeterministicContractID("contract1"),
					ContractID: "contract1",
					Type:       "unknown",
					Name:       &seedName,
					Symbol:     &seedSymbol,
					Decimals:   0,
				},
			})
		})
		require.NoError(t, err)

		// Classification succeeded but on-chain name/symbol are absent: send nil pointers.
		err = db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchUpdateMetadata(ctx, dbTx, []*Contract{
				{
					ID:       DeterministicContractID("contract1"),
					Type:     "sep41",
					Name:     nil,
					Symbol:   nil,
					Decimals: 7,
				},
			})
		})
		require.NoError(t, err)

		stored, err := db.QueryOne[Contract](ctx, dbConnectionPool, `SELECT * FROM contract_tokens WHERE contract_id = $1`, "contract1")
		require.NoError(t, err)
		require.Equal(t, "sep41", stored.Type)
		require.Nil(t, stored.Name)
		require.Nil(t, stored.Symbol)
		require.Equal(t, uint32(7), stored.Decimals)

		cleanUpDB()
	})

	t.Run("silently skips non-existent ids without inserting", func(t *testing.T) {
		cleanUpDB()

		m := &ContractModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		ghostName := "Ghost"
		ghostSymbol := "GHOST"
		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchUpdateMetadata(ctx, dbTx, []*Contract{
				{
					ID:       DeterministicContractID("does_not_exist"),
					Type:     "sep41",
					Name:     &ghostName,
					Symbol:   &ghostSymbol,
					Decimals: 7,
				},
			})
		})
		require.NoError(t, err)

		count, err := db.QueryOne[int](ctx, dbConnectionPool, `SELECT COUNT(*)::int FROM contract_tokens`)
		require.NoError(t, err)
		require.Equal(t, 0, count)

		cleanUpDB()
	})

	t.Run("only updates existing ids in mixed batch", func(t *testing.T) {
		cleanUpDB()

		m := &ContractModel{
			DB:      dbConnectionPool,
			Metrics: dbMetrics,
		}

		origName := "Original"
		origSymbol := "ORIG"
		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchInsert(ctx, dbTx, []*Contract{
				{
					ID:         DeterministicContractID("contract1"),
					ContractID: "contract1",
					Type:       "unknown",
					Name:       &origName,
					Symbol:     &origSymbol,
					Decimals:   0,
				},
			})
		})
		require.NoError(t, err)

		updatedName := "Updated"
		updatedSymbol := "UPD"
		ghostName := "Ghost"
		ghostSymbol := "GHOST"
		err = db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return m.BatchUpdateMetadata(ctx, dbTx, []*Contract{
				{
					ID:       DeterministicContractID("contract1"),
					Type:     "sep41",
					Name:     &updatedName,
					Symbol:   &updatedSymbol,
					Decimals: 7,
				},
				{
					ID:       DeterministicContractID("missing_contract"),
					Type:     "sep41",
					Name:     &ghostName,
					Symbol:   &ghostSymbol,
					Decimals: 18,
				},
			})
		})
		require.NoError(t, err)

		count, err := db.QueryOne[int](ctx, dbConnectionPool, `SELECT COUNT(*)::int FROM contract_tokens`)
		require.NoError(t, err)
		require.Equal(t, 1, count)

		stored, err := db.QueryOne[Contract](ctx, dbConnectionPool, `SELECT * FROM contract_tokens WHERE contract_id = $1`, "contract1")
		require.NoError(t, err)
		require.Equal(t, "sep41", stored.Type)
		require.Equal(t, "Updated", *stored.Name)
		require.Equal(t, "UPD", *stored.Symbol)
		require.Equal(t, uint32(7), stored.Decimals)

		cleanUpDB()
	})
}
