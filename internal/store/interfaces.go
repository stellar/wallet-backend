package store

import (
	"context"
)

// TokenContractStore defines the interface for interactions with the contract ID cache
type TokenContractStore interface {
	UpsertWithTx(ctx context.Context, contractID string, name string, symbol string) error
	Name(ctx context.Context, contractID string) (string, error)
	Symbol(ctx context.Context, contractID string) (string, error)
	Exists(ctx context.Context, contractID string) bool
}

type AccountsStore interface {
	Add(accountID string)
	Remove(accountID string)
	Exists(accountID string) bool
}
