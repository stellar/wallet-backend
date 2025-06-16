package store

import (
	"context"
	
	"github.com/stellar/wallet-backend/internal/db"
)

// ContractStore defines the interface for interactions with the contract ID cache
type ContractStore interface {
	InsertWithTx(ctx context.Context, tx db.Transaction, contractID string, name string, symbol string) error
	UpdateWithTx(ctx context.Context, tx db.Transaction, contractID string, name string, symbol string) error
	Name(ctx context.Context, contractID string) (string, error)
	Symbol(ctx context.Context, contractID string) (string, error)
	Exists(ctx context.Context, contractID string) (bool, error)
}
