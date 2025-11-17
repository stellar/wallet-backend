package data

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

type ContractModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

type Contract struct {
	ID        string    `db:"id" json:"id"`
	Name      string    `db:"name" json:"name"`
	Symbol    string    `db:"symbol" json:"symbol"`
	CreatedAt time.Time `db:"created_at" json:"createdAt"`
	UpdatedAt time.Time `db:"updated_at" json:"updatedAt"`
}

func (m *ContractModel) GetByID(ctx context.Context, contractID string) (*Contract, error) {
	start := time.Now()
	contract := &Contract{}
	err := m.DB.GetContext(ctx, contract, "SELECT * FROM contract_tokens WHERE id = $1", contractID)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("GetByID", "contract_tokens", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetByID", "contract_tokens", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting contract by ID %s: %w", contractID, err)
	}
	m.MetricsService.IncDBQuery("GetByID", "contract_tokens")
	return contract, nil
}

func (m *ContractModel) Insert(ctx context.Context, tx db.Transaction, contract *Contract) error {
	start := time.Now()
	query := `
		INSERT INTO contract_tokens (id, name, symbol)
		VALUES (:id, :name, :symbol)
	`
	_, err := tx.NamedExecContext(ctx, query, contract)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("Insert", "contract_tokens", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("Insert", "contract_tokens", utils.GetDBErrorType(err))
		return fmt.Errorf("inserting contract %s: %w", contract.ID, err)
	}
	m.MetricsService.IncDBQuery("Insert", "contract_tokens")
	return nil
}

func (m *ContractModel) Update(ctx context.Context, tx db.Transaction, contract *Contract) error {
	start := time.Now()
	query := `
		UPDATE contract_tokens
		SET name = :name, symbol = :symbol
		WHERE id = :id
	`
	_, err := tx.NamedExecContext(ctx, query, contract)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("Update", "contract_tokens", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("Update", "contract_tokens", utils.GetDBErrorType(err))
		return fmt.Errorf("updating contract %s: %w", contract.ID, err)
	}
	m.MetricsService.IncDBQuery("Update", "contract_tokens")
	return nil
}
