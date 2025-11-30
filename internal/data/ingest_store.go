package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

type IngestStoreModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

func (m *IngestStoreModel) Get(ctx context.Context, cursorName string) (uint32, error) {
	var lastSyncedLedger uint32
	start := time.Now()
	err := m.DB.GetContext(ctx, &lastSyncedLedger, `SELECT value FROM ingest_store WHERE key = $1`, cursorName)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("GetLatestLedgerSynced", "ingest_store", duration)
	// First run, key does not exist yet
	if errors.Is(err, sql.ErrNoRows) {
		m.MetricsService.IncDBQuery("GetLatestLedgerSynced", "ingest_store")
		return 0, nil
	}
	if err != nil {
		m.MetricsService.IncDBQueryError("GetLatestLedgerSynced", "ingest_store", utils.GetDBErrorType(err))
		return 0, fmt.Errorf("getting latest ledger synced for cursor %s: %w", cursorName, err)
	}
	m.MetricsService.IncDBQuery("GetLatestLedgerSynced", "ingest_store")

	return lastSyncedLedger, nil
}

func (m *IngestStoreModel) Update(ctx context.Context, dbTx db.Transaction, cursorName string, ledger uint32) error {
	const query = `
		INSERT INTO ingest_store (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value
	`
	start := time.Now()
	_, err := dbTx.ExecContext(ctx, query, cursorName, ledger)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("UpdateLatestLedgerSynced", "ingest_store", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("UpdateLatestLedgerSynced", "ingest_store", utils.GetDBErrorType(err))
		return fmt.Errorf("updating last synced ledger to %d: %w", ledger, err)
	}
	m.MetricsService.IncDBQuery("UpdateLatestLedgerSynced", "ingest_store")

	return nil
}
