package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// Classification and migration status constants.
const (
	StatusNotStarted = "not_started"
	StatusInProgress = "in_progress"
	StatusSuccess    = "success"
	StatusFailed     = "failed"
)

// Protocols represents a row in the protocols table.
type Protocols struct {
	ID                          string    `db:"id"`
	ClassificationStatus        string    `db:"classification_status"`
	HistoryMigrationStatus      string    `db:"history_migration_status"`
	CurrentStateMigrationStatus string    `db:"current_state_migration_status"`
	CreatedAt                   time.Time `db:"created_at"`
	UpdatedAt                   time.Time `db:"updated_at"`
}

// ProtocolsModelInterface defines the interface for protocols operations.
type ProtocolsModelInterface interface {
	UpdateClassificationStatus(ctx context.Context, dbTx pgx.Tx, protocolIDs []string, status string) error
	UpdateHistoryMigrationStatus(ctx context.Context, dbTx pgx.Tx, protocolIDs []string, status string) error
	GetByIDs(ctx context.Context, protocolIDs []string) ([]Protocols, error)
	GetClassified(ctx context.Context) ([]Protocols, error)
	InsertIfNotExists(ctx context.Context, dbTx pgx.Tx, protocolID string) error
}

// ProtocolsModel implements ProtocolsModelInterface.
type ProtocolsModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

var _ ProtocolsModelInterface = (*ProtocolsModel)(nil)

// UpdateClassificationStatus updates classification_status and updated_at for the given protocol IDs.
func (m *ProtocolsModel) UpdateClassificationStatus(ctx context.Context, dbTx pgx.Tx, protocolIDs []string, status string) error {
	if len(protocolIDs) == 0 {
		return nil
	}

	const query = `
		UPDATE protocols
		SET classification_status = $1, updated_at = NOW()
		WHERE id = ANY($2)
	`

	start := time.Now()
	_, err := dbTx.Exec(ctx, query, status, protocolIDs)
	if err != nil {
		m.MetricsService.IncDBQueryError("UpdateClassificationStatus", "protocols", utils.GetDBErrorType(err))
		return fmt.Errorf("updating classification status for protocols: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("UpdateClassificationStatus", "protocols", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("UpdateClassificationStatus", "protocols")
	return nil
}

// UpdateHistoryMigrationStatus updates history_migration_status and updated_at for the given protocol IDs.
func (m *ProtocolsModel) UpdateHistoryMigrationStatus(ctx context.Context, dbTx pgx.Tx, protocolIDs []string, status string) error {
	if len(protocolIDs) == 0 {
		return nil
	}

	const query = `
		UPDATE protocols
		SET history_migration_status = $1, updated_at = NOW()
		WHERE id = ANY($2)
	`

	start := time.Now()
	_, err := dbTx.Exec(ctx, query, status, protocolIDs)
	if err != nil {
		m.MetricsService.IncDBQueryError("UpdateHistoryMigrationStatus", "protocols", utils.GetDBErrorType(err))
		return fmt.Errorf("updating history migration status for protocols: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("UpdateHistoryMigrationStatus", "protocols", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("UpdateHistoryMigrationStatus", "protocols")
	return nil
}

// GetByIDs returns protocols matching the given IDs.
func (m *ProtocolsModel) GetByIDs(ctx context.Context, protocolIDs []string) ([]Protocols, error) {
	if len(protocolIDs) == 0 {
		return nil, nil
	}

	const query = `
		SELECT id, classification_status, history_migration_status, current_state_migration_status, created_at, updated_at
		FROM protocols
		WHERE id = ANY($1)
	`

	start := time.Now()
	rows, err := m.DB.PgxPool().Query(ctx, query, protocolIDs)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetByIDs", "protocols", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("querying protocols by IDs: %w", err)
	}
	defer rows.Close()

	var protocols []Protocols
	for rows.Next() {
		var p Protocols
		if err := rows.Scan(&p.ID, &p.ClassificationStatus, &p.HistoryMigrationStatus, &p.CurrentStateMigrationStatus, &p.CreatedAt, &p.UpdatedAt); err != nil {
			m.MetricsService.IncDBQueryError("GetByIDs", "protocols", utils.GetDBErrorType(err))
			return nil, fmt.Errorf("scanning protocol row: %w", err)
		}
		protocols = append(protocols, p)
	}
	if err := rows.Err(); err != nil {
		m.MetricsService.IncDBQueryError("GetByIDs", "protocols", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("iterating protocol rows: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("GetByIDs", "protocols", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("GetByIDs", "protocols")
	return protocols, nil
}

// GetClassified returns all protocols with classification_status = 'success'.
func (m *ProtocolsModel) GetClassified(ctx context.Context) ([]Protocols, error) {
	const query = `
		SELECT id, classification_status, history_migration_status, current_state_migration_status, created_at, updated_at
		FROM protocols
		WHERE classification_status = 'success'
	`

	start := time.Now()
	rows, err := m.DB.PgxPool().Query(ctx, query)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetClassified", "protocols", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("querying classified protocols: %w", err)
	}
	defer rows.Close()

	var protocols []Protocols
	for rows.Next() {
		var p Protocols
		if err := rows.Scan(&p.ID, &p.ClassificationStatus, &p.HistoryMigrationStatus, &p.CurrentStateMigrationStatus, &p.CreatedAt, &p.UpdatedAt); err != nil {
			m.MetricsService.IncDBQueryError("GetClassified", "protocols", utils.GetDBErrorType(err))
			return nil, fmt.Errorf("scanning classified protocol row: %w", err)
		}
		protocols = append(protocols, p)
	}
	if err := rows.Err(); err != nil {
		m.MetricsService.IncDBQueryError("GetClassified", "protocols", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("iterating classified protocol rows: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("GetClassified", "protocols", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("GetClassified", "protocols")
	return protocols, nil
}

// InsertIfNotExists inserts a protocol if it doesn't already exist (idempotent).
func (m *ProtocolsModel) InsertIfNotExists(ctx context.Context, dbTx pgx.Tx, protocolID string) error {
	const query = `INSERT INTO protocols (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`

	start := time.Now()
	_, err := dbTx.Exec(ctx, query, protocolID)
	if err != nil {
		m.MetricsService.IncDBQueryError("InsertIfNotExists", "protocols", utils.GetDBErrorType(err))
		return fmt.Errorf("inserting protocol %s: %w", protocolID, err)
	}

	m.MetricsService.ObserveDBQueryDuration("InsertIfNotExists", "protocols", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("InsertIfNotExists", "protocols")
	return nil
}
