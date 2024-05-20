package data

import (
	"context"
	"fmt"

	"github.com/stellar/wallet-backend/internal/db"
)

type PaymentModel struct {
	db db.ConnectionPool
}

func (m *PaymentModel) SubscribeAddress(ctx context.Context, address string) error {
	const query = `INSERT INTO accounts (stellar_address) VALUES ($1) ON CONFLICT DO NOTHING`
	_, err := m.db.ExecContext(ctx, query, address)
	if err != nil {
		return fmt.Errorf("subscribing address %s to payments tracking: %w", address, err)
	}

	return nil
}

func (m *PaymentModel) UnsubscribeAddress(ctx context.Context, address string) error {
	const query = `DELETE FROM accounts WHERE stellar_address = $1`
	_, err := m.db.ExecContext(ctx, query, address)
	if err != nil {
		return fmt.Errorf("unsubscribing address %s to payments tracking: %w", address, err)
	}

	return nil
}
