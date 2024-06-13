package data

import (
	"context"
	"fmt"

	"github.com/stellar/wallet-backend/internal/db"
)

type AccountModel struct {
	DB db.ConnectionPool
}

func (m *AccountModel) Insert(ctx context.Context, address string) error {
	const query = `INSERT INTO accounts (stellar_address) VALUES ($1) ON CONFLICT DO NOTHING`
	_, err := m.DB.ExecContext(ctx, query, address)
	if err != nil {
		return fmt.Errorf("inserting address %s: %w", address, err)
	}

	return nil
}

func (m *AccountModel) Delete(ctx context.Context, address string) error {
	const query = `DELETE FROM accounts WHERE stellar_address = $1`
	_, err := m.DB.ExecContext(ctx, query, address)
	if err != nil {
		return fmt.Errorf("deleting address %s: %w", address, err)
	}

	return nil
}

func (m *AccountModel) IsAccountFeeBumpEligible(ctx context.Context, address string) (bool, error) {
	const query = `
		SELECT 
			EXISTS(
				SELECT stellar_address FROM accounts WHERE stellar_address = $1
				UNION
				SELECT public_key FROM channel_accounts WHERE public_key = $1
			)
	`
	var exists bool
	err := m.DB.GetContext(ctx, &exists, query, address)
	if err != nil {
		return false, fmt.Errorf("checking if account %s is fee bump eligible: %w", address, err)
	}

	return exists, nil
}
