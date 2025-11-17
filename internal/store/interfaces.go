package store

import (
	"context"
)

// ContractTokenStore defines the interface for interactions with the contract ID cache
type ContractTokenStore interface {
	UpsertWithTx(ctx context.Context, contractID string, name string, symbol string) error
	Name(ctx context.Context, contractID string) (string, error)
	Symbol(ctx context.Context, contractID string) (string, error)
	Exists(ctx context.Context, contractID string) bool
}
