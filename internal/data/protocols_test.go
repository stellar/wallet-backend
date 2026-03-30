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
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestProtocolsModel(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.Exec(ctx, `DELETE FROM protocols`)
		require.NoError(t, err)
	}

	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB

	t.Run("InsertIfNotExists inserts new protocol", func(t *testing.T) {
		cleanUpDB()

		model := &ProtocolsModel{DB: dbConnectionPool, Metrics: dbMetrics}
		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.InsertIfNotExists(ctx, dbTx, "SEP41")
		})
		require.NoError(t, err)

		var count int
		err = dbConnectionPool.QueryRow(ctx, `SELECT COUNT(*) FROM protocols WHERE id = 'SEP41'`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("InsertIfNotExists is idempotent", func(t *testing.T) {
		cleanUpDB()

		model := &ProtocolsModel{DB: dbConnectionPool, Metrics: dbMetrics}
		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			if err := model.InsertIfNotExists(ctx, dbTx, "SEP41"); err != nil {
				return err
			}
			return model.InsertIfNotExists(ctx, dbTx, "SEP41")
		})
		require.NoError(t, err)

		var count int
		err = dbConnectionPool.QueryRow(ctx, `SELECT COUNT(*) FROM protocols WHERE id = 'SEP41'`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("UpdateClassificationStatus updates status", func(t *testing.T) {
		cleanUpDB()

		model := &ProtocolsModel{DB: dbConnectionPool, Metrics: dbMetrics}

		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.InsertIfNotExists(ctx, dbTx, "SEP41")
		})
		require.NoError(t, err)

		err = db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.UpdateClassificationStatus(ctx, dbTx, []string{"SEP41"}, StatusInProgress)
		})
		require.NoError(t, err)

		var status string
		err = dbConnectionPool.QueryRow(ctx, `SELECT classification_status FROM protocols WHERE id = 'SEP41'`).Scan(&status)
		require.NoError(t, err)
		assert.Equal(t, StatusInProgress, status)
	})

	t.Run("GetByIDs returns matching protocols", func(t *testing.T) {
		cleanUpDB()

		model := &ProtocolsModel{DB: dbConnectionPool, Metrics: dbMetrics}

		err := db.RunInTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			if err := model.InsertIfNotExists(ctx, dbTx, "SEP41"); err != nil {
				return err
			}
			return model.InsertIfNotExists(ctx, dbTx, "BLEND")
		})
		require.NoError(t, err)

		protocols, err := model.GetByIDs(ctx, []string{"SEP41", "BLEND"})
		require.NoError(t, err)
		assert.Len(t, protocols, 2)

		protocols, err = model.GetByIDs(ctx, []string{"SEP41"})
		require.NoError(t, err)
		assert.Len(t, protocols, 1)
		assert.Equal(t, "SEP41", protocols[0].ID)
		assert.Equal(t, StatusNotStarted, protocols[0].ClassificationStatus)
	})

	t.Run("GetByIDs with empty slice returns nil", func(t *testing.T) {
		model := &ProtocolsModel{DB: dbConnectionPool, Metrics: dbMetrics}

		protocols, err := model.GetByIDs(ctx, []string{})
		require.NoError(t, err)
		assert.Nil(t, protocols)
	})
}
