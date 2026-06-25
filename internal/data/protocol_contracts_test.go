package data

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestProtocolContractsBatchInsert(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB

	insertWasms := func(t *testing.T, hashes ...types.HashBytea) {
		t.Helper()
		wasmModel := &ProtocolWasmsModel{DB: dbConnectionPool, Metrics: dbMetrics}
		wasms := make([]ProtocolWasms, len(hashes))
		for i, h := range hashes {
			wasms[i] = ProtocolWasms{WasmHash: h}
		}
		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return wasmModel.BatchInsert(ctx, dbTx, wasms)
		})
		require.NoError(t, err)
	}

	cleanUpDB := func() {
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM protocol_contracts`)
		require.NoError(t, err)
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM protocol_wasms`)
		require.NoError(t, err)
	}

	t.Run("empty input returns no error", func(t *testing.T) {
		cleanUpDB()

		model := &ProtocolContractsModel{DB: dbConnectionPool, Metrics: dbMetrics}
		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{})
		})
		assert.NoError(t, err)
	})

	t.Run("single insert", func(t *testing.T) {
		cleanUpDB()

		wasmHash := types.HashBytea("abc123def4560000000000000000000000000000000000000000000000000000")
		insertWasms(t, wasmHash)

		contractID := types.HashBytea("0100000000000000000000000000000000000000000000000000000000000000")
		model := &ProtocolContractsModel{DB: dbConnectionPool, Metrics: dbMetrics}
		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{
				{ContractID: contractID, WasmHash: wasmHash},
			})
		})
		assert.NoError(t, err)

		var count int
		cidBytes, err := contractID.Value()
		require.NoError(t, err)
		err = dbConnectionPool.QueryRow(ctx, `SELECT COUNT(*) FROM protocol_contracts WHERE contract_id = $1`, cidBytes).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("multiple inserts", func(t *testing.T) {
		cleanUpDB()

		wh1 := types.HashBytea("0100000000000000000000000000000000000000000000000000000000000000")
		wh2 := types.HashBytea("0200000000000000000000000000000000000000000000000000000000000000")
		insertWasms(t, wh1, wh2)

		model := &ProtocolContractsModel{DB: dbConnectionPool, Metrics: dbMetrics}
		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{
				{ContractID: "aa00000000000000000000000000000000000000000000000000000000000000", WasmHash: wh1},
				{ContractID: "bb00000000000000000000000000000000000000000000000000000000000000", WasmHash: wh2},
				{ContractID: "cc00000000000000000000000000000000000000000000000000000000000000", WasmHash: wh1},
			})
		})
		assert.NoError(t, err)

		var count int
		err = dbConnectionPool.QueryRow(ctx, `SELECT COUNT(*) FROM protocol_contracts`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 3, count)
	})

	t.Run("upsert updates wasm_hash", func(t *testing.T) {
		cleanUpDB()

		wh1 := types.HashBytea("0100000000000000000000000000000000000000000000000000000000000000")
		wh2 := types.HashBytea("0200000000000000000000000000000000000000000000000000000000000000")
		insertWasms(t, wh1, wh2)

		contractID := types.HashBytea("aa00000000000000000000000000000000000000000000000000000000000000")
		model := &ProtocolContractsModel{DB: dbConnectionPool, Metrics: dbMetrics}

		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{
				{ContractID: contractID, WasmHash: wh1},
			})
		})
		require.NoError(t, err)

		err = db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{
				{ContractID: contractID, WasmHash: wh2},
			})
		})
		require.NoError(t, err)

		cidBytes, err := contractID.Value()
		require.NoError(t, err)
		wh2Bytes, err := wh2.Value()
		require.NoError(t, err)

		var storedWasmHash []byte
		err = dbConnectionPool.QueryRow(ctx, `SELECT wasm_hash FROM protocol_contracts WHERE contract_id = $1`, cidBytes).Scan(&storedWasmHash)
		require.NoError(t, err)
		assert.Equal(t, wh2Bytes.([]byte), storedWasmHash)
	})

	t.Run("upsert preserves name when new value is NULL", func(t *testing.T) {
		cleanUpDB()

		wh1 := types.HashBytea("0100000000000000000000000000000000000000000000000000000000000000")
		wh2 := types.HashBytea("0200000000000000000000000000000000000000000000000000000000000000")
		insertWasms(t, wh1, wh2)

		contractID := types.HashBytea("aa00000000000000000000000000000000000000000000000000000000000000")
		contractName := "my-contract"
		model := &ProtocolContractsModel{DB: dbConnectionPool, Metrics: dbMetrics}

		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{
				{ContractID: contractID, WasmHash: wh1, Name: &contractName},
			})
		})
		require.NoError(t, err)

		err = db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{
				{ContractID: contractID, WasmHash: wh2, Name: nil},
			})
		})
		require.NoError(t, err)

		cidBytes, err := contractID.Value()
		require.NoError(t, err)
		wh2Bytes, err := wh2.Value()
		require.NoError(t, err)

		var storedName *string
		var storedWasmHash []byte
		err = dbConnectionPool.QueryRow(ctx, `SELECT name FROM protocol_contracts WHERE contract_id = $1`, cidBytes).Scan(&storedName)
		require.NoError(t, err)
		require.NotNil(t, storedName)
		assert.Equal(t, "my-contract", *storedName)

		err = dbConnectionPool.QueryRow(ctx, `SELECT wasm_hash FROM protocol_contracts WHERE contract_id = $1`, cidBytes).Scan(&storedWasmHash)
		require.NoError(t, err)
		assert.Equal(t, wh2Bytes.([]byte), storedWasmHash)
	})

	t.Run("FK enforcement silently skips missing wasm", func(t *testing.T) {
		cleanUpDB()

		model := &ProtocolContractsModel{DB: dbConnectionPool, Metrics: dbMetrics}
		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{
				{
					ContractID: "aa00000000000000000000000000000000000000000000000000000000000000",
					WasmHash:   "dead000000000000000000000000000000000000000000000000000000000000",
				},
			})
		})
		assert.NoError(t, err)

		var count int
		err = dbConnectionPool.QueryRow(ctx, `SELECT COUNT(*) FROM protocol_contracts`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

func TestProtocolContractsBatchGetByContractIDs(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB
	model := &ProtocolContractsModel{DB: dbConnectionPool, Metrics: dbMetrics}

	insertWasm := func(t *testing.T, hash types.HashBytea, protocolID *string) {
		t.Helper()
		if protocolID != nil {
			_, pErr := dbConnectionPool.Exec(ctx,
				`INSERT INTO protocols (id) VALUES ($1) ON CONFLICT DO NOTHING`, *protocolID)
			require.NoError(t, pErr)
		}
		hb, vErr := hash.Value()
		require.NoError(t, vErr)
		_, eErr := dbConnectionPool.Exec(ctx,
			`INSERT INTO protocol_wasms (wasm_hash, protocol_id) VALUES ($1, $2)`, hb, protocolID)
		require.NoError(t, eErr)
	}

	cleanUpDB := func() {
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM protocol_contracts`)
		require.NoError(t, err)
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM protocol_wasms`)
		require.NoError(t, err)
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM protocols`)
		require.NoError(t, err)
	}

	whSEP := types.HashBytea("aa00000000000000000000000000000000000000000000000000000000000000")
	whOther := types.HashBytea("bb00000000000000000000000000000000000000000000000000000000000000")
	whNull := types.HashBytea("cc00000000000000000000000000000000000000000000000000000000000000")
	cA := types.HashBytea("a100000000000000000000000000000000000000000000000000000000000000")
	cB := types.HashBytea("b100000000000000000000000000000000000000000000000000000000000000")
	cC := types.HashBytea("c100000000000000000000000000000000000000000000000000000000000000")
	cD := types.HashBytea("d100000000000000000000000000000000000000000000000000000000000000")
	cMissing := types.HashBytea("e100000000000000000000000000000000000000000000000000000000000000")

	t.Run("empty input returns nil", func(t *testing.T) {
		cleanUpDB()
		result, qErr := model.BatchGetByContractIDs(ctx, nil)
		require.NoError(t, qErr)
		assert.Nil(t, result)
	})

	t.Run("groups by protocol, excludes unclassified and unmatched", func(t *testing.T) {
		cleanUpDB()
		sep, other := "SEP41", "OTHER"
		insertWasm(t, whSEP, &sep)
		insertWasm(t, whOther, &other)
		insertWasm(t, whNull, nil)

		err = db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.BatchInsert(ctx, dbTx, []ProtocolContracts{
				{ContractID: cA, WasmHash: whSEP},
				{ContractID: cB, WasmHash: whOther},
				{ContractID: cC, WasmHash: whNull},
				{ContractID: cD, WasmHash: whSEP},
			})
		})
		require.NoError(t, err)

		// Query a subset: cA (SEP41), cB (OTHER), cC (unclassified), cMissing (absent). cD is not queried.
		result, qErr := model.BatchGetByContractIDs(ctx, []types.HashBytea{cA, cB, cC, cMissing})
		require.NoError(t, qErr)

		require.Len(t, result, 2)
		require.Len(t, result["SEP41"], 1)
		assert.Equal(t, cA, result["SEP41"][0].ContractID)
		require.Len(t, result["OTHER"], 1)
		assert.Equal(t, cB, result["OTHER"][0].ContractID)
	})
}
