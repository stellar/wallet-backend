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
	contract := &Contract{}
	err := m.DB.GetContext(ctx, contract, "SELECT * FROM contracts WHERE id = $1", contractID)
	if err != nil {
		return nil, fmt.Errorf("getting contract by ID %s: %w", contractID, err)
	}
	return contract, nil
}

func (m *ContractModel) Insert(ctx context.Context, tx db.Transaction, contract *Contract) error {
	query := `
		INSERT INTO contracts (id, name, symbol)
		VALUES (:id, :name, :symbol)
	`
	_, err := tx.NamedExecContext(ctx, query, contract)
	if err != nil {
		return fmt.Errorf("inserting contract %s: %w", contract.ID, err)
	}
	return nil
}

func (m *ContractModel) Update(ctx context.Context, tx db.Transaction, contract *Contract) error {
	query := `
		UPDATE contracts
		SET name = :name, symbol = :symbol
		WHERE id = :id
	`
	_, err := tx.NamedExecContext(ctx, query, contract)
	if err != nil {
		return fmt.Errorf("updating contract %s: %w", contract.ID, err)
	}
	return nil
}

func (m *ContractModel) GetAll(ctx context.Context) ([]*Contract, error) {
	contracts := []*Contract{}
	err := m.DB.SelectContext(ctx, &contracts, "SELECT * FROM contracts")
	if err != nil {
		return nil, fmt.Errorf("getting all contracts: %w", err)
	}
	return contracts, nil
}
