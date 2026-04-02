package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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
	GetByIDs(ctx context.Context, protocolIDs []string) ([]Protocols, error)
	GetClassified(ctx context.Context) ([]Protocols, error)
	InsertIfNotExists(ctx context.Context, dbTx pgx.Tx, protocolID string) error
}

// ProtocolsModel implements ProtocolsModelInterface.
type ProtocolsModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
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
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("UpdateClassificationStatus", "protocols").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("UpdateClassificationStatus", "protocols").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("UpdateClassificationStatus", "protocols", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("updating classification status for protocols: %w", err)
	}
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
	protocols, err := db.QueryMany[Protocols](ctx, m.DB, query, protocolIDs)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetByIDs", "protocols").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetByIDs", "protocols").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetByIDs", "protocols", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying protocols by IDs: %w", err)
	}
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
	protocols, err := db.QueryMany[Protocols](ctx, m.DB, query)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("GetClassified", "protocols").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("GetClassified", "protocols").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetClassified", "protocols", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying classified protocols: %w", err)
	}
	return protocols, nil
}

// InsertIfNotExists inserts a protocol if it doesn't already exist (idempotent).
func (m *ProtocolsModel) InsertIfNotExists(ctx context.Context, dbTx pgx.Tx, protocolID string) error {
	const query = `INSERT INTO protocols (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`

	start := time.Now()
	_, err := dbTx.Exec(ctx, query, protocolID)
	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("InsertIfNotExists", "protocols").Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("InsertIfNotExists", "protocols").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("InsertIfNotExists", "protocols", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("inserting protocol %s: %w", protocolID, err)
	}
	return nil
}
