package data

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
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
	err := m.DB.GetContext(ctx, contract, "SELECT * FROM token_contracts WHERE id = $1", contractID)
	if err != nil {
		return nil, fmt.Errorf("getting contract by ID %s: %w", contractID, err)
	}
	m.MetricsService.ObserveDBQueryDuration("SELECT", "token_contracts", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("SELECT", "token_contracts")
	return contract, nil
}

func (m *ContractModel) Insert(ctx context.Context, tx db.Transaction, contract *Contract) error {
	start := time.Now()
	query := `
		INSERT INTO token_contracts (id, name, symbol)
		VALUES (:id, :name, :symbol)
	`
	_, err := tx.NamedExecContext(ctx, query, contract)
	if err != nil {
		return fmt.Errorf("inserting contract %s: %w", contract.ID, err)
	}
	m.MetricsService.ObserveDBQueryDuration("INSERT", "token_contracts", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("INSERT", "token_contracts")
	return nil
}

func (m *ContractModel) Update(ctx context.Context, tx db.Transaction, contract *Contract) error {
	start := time.Now()
	query := `
		UPDATE token_contracts
		SET name = :name, symbol = :symbol
		WHERE id = :id
	`
	_, err := tx.NamedExecContext(ctx, query, contract)
	if err != nil {
		return fmt.Errorf("updating contract %s: %w", contract.ID, err)
	}
	m.MetricsService.ObserveDBQueryDuration("UPDATE", "token_contracts", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("UPDATE", "token_contracts")
	return nil
}

func (m *ContractModel) GetAll(ctx context.Context) ([]*Contract, error) {
	start := time.Now()
	contracts := []*Contract{}
	err := m.DB.SelectContext(ctx, &contracts, "SELECT * FROM token_contracts")
	if err != nil {
		return nil, fmt.Errorf("getting all contracts: %w", err)
	}
	m.MetricsService.ObserveDBQueryDuration("SELECT", "token_contracts", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("SELECT", "token_contracts")
	return contracts, nil
}
