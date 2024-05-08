package data

import (
	"context"
	"wallet-backend/internal/db"

	"github.com/stellar/go/support/errors"
)

type PaymentModel struct {
	db db.ConnectionPool
}

func (m *PaymentModel) SubscribeAddress(ctx context.Context, address string) error {
	const query = `INSERT INTO accounts (stellar_address) VALUES ($1) ON CONFLICT DO NOTHING`
	_, err := m.db.ExecContext(ctx, query, address)
	if err != nil {
		return errors.Wrapf(err, "subscribing address %s to payments tracking", address)
	}

	return nil
}
