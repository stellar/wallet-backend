package data

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestProtocolsModel(t *testing.T) {
	ctx := context.Background()

	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	cleanUpDB := func() {
		_, err = dbConnectionPool.ExecContext(ctx, `DELETE FROM protocols`)
		require.NoError(t, err)
	}

	t.Run("InsertIfNotExists inserts new protocol", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		model := &ProtocolsModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.InsertIfNotExists(ctx, dbTx, "SEP41")
		})
		require.NoError(t, err)

		// Verify
		var count int
		err = dbConnectionPool.GetContext(ctx, &count, `SELECT COUNT(*) FROM protocols WHERE id = 'SEP41'`)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("InsertIfNotExists is idempotent", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		model := &ProtocolsModel{DB: dbConnectionPool, MetricsService: mockMetricsService}
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			if err := model.InsertIfNotExists(ctx, dbTx, "SEP41"); err != nil {
				return err
			}
			return model.InsertIfNotExists(ctx, dbTx, "SEP41")
		})
		require.NoError(t, err)

		var count int
		err = dbConnectionPool.GetContext(ctx, &count, `SELECT COUNT(*) FROM protocols WHERE id = 'SEP41'`)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("UpdateClassificationStatus updates status", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		model := &ProtocolsModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		// Insert protocol first
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.InsertIfNotExists(ctx, dbTx, "SEP41")
		})
		require.NoError(t, err)

		// Update status
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.UpdateClassificationStatus(ctx, dbTx, []string{"SEP41"}, StatusInProgress)
		})
		require.NoError(t, err)

		// Verify
		var status string
		err = dbConnectionPool.GetContext(ctx, &status, `SELECT classification_status FROM protocols WHERE id = 'SEP41'`)
		require.NoError(t, err)
		assert.Equal(t, StatusInProgress, status)
	})

	t.Run("UpdateHistoryMigrationStatus updates status", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		model := &ProtocolsModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		// Insert protocol first
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.InsertIfNotExists(ctx, dbTx, "SEP41")
		})
		require.NoError(t, err)

		// Update status
		err = db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			return model.UpdateHistoryMigrationStatus(ctx, dbTx, []string{"SEP41"}, StatusInProgress)
		})
		require.NoError(t, err)

		// Verify
		var status string
		err = dbConnectionPool.GetContext(ctx, &status, `SELECT history_migration_status FROM protocols WHERE id = 'SEP41'`)
		require.NoError(t, err)
		assert.Equal(t, StatusInProgress, status)
	})

	t.Run("GetByIDs returns matching protocols", func(t *testing.T) {
		cleanUpDB()
		mockMetricsService := metrics.NewMockMetricsService()
		mockMetricsService.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
		mockMetricsService.On("IncDBQuery", mock.Anything, mock.Anything).Return()
		defer mockMetricsService.AssertExpectations(t)

		model := &ProtocolsModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		// Insert two protocols
		err := db.RunInPgxTransaction(ctx, dbConnectionPool, func(dbTx pgx.Tx) error {
			if err := model.InsertIfNotExists(ctx, dbTx, "SEP41"); err != nil {
				return err
			}
			return model.InsertIfNotExists(ctx, dbTx, "BLEND")
		})
		require.NoError(t, err)

		// Get by IDs
		protocols, err := model.GetByIDs(ctx, []string{"SEP41", "BLEND"})
		require.NoError(t, err)
		assert.Len(t, protocols, 2)

		// Get single
		protocols, err = model.GetByIDs(ctx, []string{"SEP41"})
		require.NoError(t, err)
		assert.Len(t, protocols, 1)
		assert.Equal(t, "SEP41", protocols[0].ID)
		assert.Equal(t, StatusNotStarted, protocols[0].ClassificationStatus)
	})

	t.Run("GetByIDs with empty slice returns nil", func(t *testing.T) {
		mockMetricsService := metrics.NewMockMetricsService()
		model := &ProtocolsModel{DB: dbConnectionPool, MetricsService: mockMetricsService}

		protocols, err := model.GetByIDs(ctx, []string{})
		require.NoError(t, err)
		assert.Nil(t, protocols)
	})
}
