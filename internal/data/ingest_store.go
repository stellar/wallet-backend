package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
)

type IngestStoreModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

func (m *IngestStoreModel) GetLatestLedgerSynced(ctx context.Context, cursorName string) (uint32, error) {
	var lastSyncedLedger uint32
	start := time.Now()
	err := m.DB.GetContext(ctx, &lastSyncedLedger, `SELECT value FROM ingest_store WHERE key = $1`, cursorName)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "ingest_store", duration)
	m.MetricsService.IncDBQuery("SELECT", "ingest_store")
	// First run, key does not exist yet
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("getting latest ledger synced for cursor %s: %w", cursorName, err)
	}

	return lastSyncedLedger, nil
}

func (m *IngestStoreModel) UpdateLatestLedgerSynced(ctx context.Context, cursorName string, ledger uint32) error {
	const query = `
		INSERT INTO ingest_store (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value
	`
	start := time.Now()
	_, err := m.DB.ExecContext(ctx, query, cursorName, ledger)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("INSERT", "ingest_store", duration)
	if err != nil {
		return fmt.Errorf("updating last synced ledger to %d: %w", ledger, err)
	}
	m.MetricsService.IncDBQuery("INSERT", "ingest_store")

	return nil
}
